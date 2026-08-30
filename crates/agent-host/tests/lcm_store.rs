use std::{collections::BTreeMap, sync::Arc};

use agent_runtime::{
    core::{
        content::Message,
        ids::TurnId,
        manifest::{
            CapabilityResolution, ContextSegmentRecord, ModelResolution, RunManifest, SegmentId,
            SegmentSensitivity, SummaryCoverage,
        },
        provider::ModelId,
        store::TurnManifest,
    },
    lcm::{
        ExpansionRequest, LcmAppendRequest, LcmClassification, LcmEntry, LcmEntryId, LcmNodeId,
        LcmOperationId, LcmRange, LcmReader, LcmSequence, LcmSourceMetadata, LcmTimelineId,
        LcmWriter, LeafCommit, SummaryProvenance, source_fingerprint_entries,
    },
    registry::{Fingerprint, RegistryRevision, TrustClass},
};
use db::{
    AgentChatRepo, AgentContextScopeRepo, AgentRepo, AgentSessionRepo, AgentStatus, CreateAgent,
    CreateAgentContextScope, CreateAgentIdentity, CreateAgentProfile, CreateAgentSession,
    CreateProject, CreateTask, ProjectRepo, RotateAgentSession, SqliteDb, TaskRepo, now_rfc3339,
};
use forge_agent_host::{
    AGENT_RUNTIME_REVISION, ContentGuardRevision, RuntimeContextManifestLink, Sensitivity,
    SqliteLcmStore, SqliteProtectedRuntimeStore, TaskLcmProjectionPolicy,
};

#[tokio::test]
async fn sqlite_lcm_is_acl_first_idempotent_and_restart_safe() {
    let pool = db::create_sqlite_pool("sqlite::memory:")
        .await
        .expect("pool");
    db::run_migrations(&pool).await.expect("migrations");
    let db = Arc::new(SqliteDb::new(pool));
    AgentRepo::create(
        &*db,
        CreateAgent {
            id: "lcm-agent".to_owned(),
            name: "LCM Agent".to_owned(),
            description: None,
            executor_type: "codex".to_owned(),
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
            owner_id: None,
            visibility: "account".to_owned(),
            created_at: "2026-08-12T00:00:00Z".to_owned(),
            updated_at: "2026-08-12T00:00:00Z".to_owned(),
        },
    )
    .await
    .expect("identity");

    let store = SqliteLcmStore::open_for_binding(
        Arc::clone(&db),
        "lcm-agent",
        "account",
        "account-1",
        "test-auth",
        "2026-08-12T00:00:00Z",
    )
    .await
    .expect("store");
    let view = store.view();
    assert_eq!(store.store_revision().as_str(), "forge-sqlite-lcm-1");
    assert_eq!(
        AGENT_RUNTIME_REVISION,
        "b3f966b0e108e6d4683c0a9c94055aaa6aa7d919"
    );

    let entry = LcmEntry::new(
        LcmTimelineId::new(store.timeline_id()),
        LcmEntryId::new("entry-0"),
        LcmSequence::new(0),
        Message::user("hello from a scoped timeline"),
        LcmSourceMetadata::new(LcmClassification::new(
            agent_runtime::context::Sensitivity::Internal,
            TrustClass::UserContent,
        )),
    );
    let append = LcmAppendRequest::new(LcmOperationId::new("append-0"), vec![entry.clone()]);
    let first = store.append(&view, append.clone()).await.expect("append");
    assert_eq!(first.revision.get(), 1);
    let replay = store.append(&view, append).await.expect("replay");
    assert!(replay.already_committed);
    assert_eq!(replay.revision.get(), 1);
    assert_eq!(
        store
            .load_range(&view, LcmRange::single(LcmSequence::new(0)), 8)
            .await
            .unwrap(),
        vec![entry.clone()]
    );

    let classification = LcmClassification::new(
        agent_runtime::context::Sensitivity::Internal,
        TrustClass::UserContent,
    );
    let node = LeafCommit {
        expected_revision: first.revision,
        operation_id: LcmOperationId::new("leaf-0"),
        node_id: LcmNodeId::new("node-0"),
        range: LcmRange::single(LcmSequence::new(0)),
        entry_ids: vec![entry.id.clone()],
        source_fingerprint: source_fingerprint_entries(std::slice::from_ref(&entry)),
        summary: "hello".to_owned(),
        token_count: 1,
        source_token_count: 8,
        policy_revision: RegistryRevision::new("policy-1"),
        algorithm_revision: RegistryRevision::new("algorithm-1"),
        sizer_revision: RegistryRevision::new("sizer-1"),
        provenance: SummaryProvenance::Deterministic {
            revision: RegistryRevision::new("deterministic-1"),
        },
        classification,
        operation_fingerprint: None,
    };
    let committed = store.commit_leaf(&view, node).await.expect("leaf");
    assert_eq!(committed.revision.get(), 2);
    let expanded = store
        .expand(&view, ExpansionRequest::new(committed.node.id.clone(), 8))
        .await
        .expect("expand");
    assert!(expanded.complete);
    assert_eq!(expanded.items.len(), 1);

    let restarted = SqliteLcmStore::open_for_binding(
        Arc::clone(&db),
        "lcm-agent",
        "account",
        "account-1",
        "test-auth",
        "2026-08-12T00:00:00Z",
    )
    .await
    .expect("restart adapter");
    let restarted_view = restarted.view();
    assert_eq!(
        restarted
            .current_revision(&restarted_view)
            .await
            .unwrap()
            .get(),
        2
    );
    assert_eq!(
        restarted.active_nodes(&restarted_view).await.unwrap().len(),
        1
    );

    let foreign_authority = agent_runtime::harness::LcmViewAuthority::new();
    let foreign_view =
        foreign_authority.issue(LcmTimelineId::new(store.timeline_id()), "test-auth");
    assert!(matches!(
        store.current_revision(&foreign_view).await,
        Err(agent_runtime::lcm::LcmError::Unauthorized)
    ));
}

#[tokio::test]
async fn task_projection_preserves_tool_pairs_and_provenance() {
    let pool = db::create_sqlite_pool("sqlite::memory:")
        .await
        .expect("pool");
    db::run_migrations(&pool).await.expect("migrations");
    let db = Arc::new(SqliteDb::new(pool));
    AgentRepo::create(
        &*db,
        CreateAgent {
            id: "task-lcm-agent".to_owned(),
            name: "Task LCM Agent".to_owned(),
            description: None,
            executor_type: "embedded".to_owned(),
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
            owner_id: None,
            visibility: "account".to_owned(),
            created_at: "2026-08-12T00:00:00Z".to_owned(),
            updated_at: "2026-08-12T00:00:00Z".to_owned(),
        },
    )
    .await
    .expect("identity");
    let store = SqliteLcmStore::open_for_binding(
        Arc::clone(&db),
        "task-lcm-agent",
        "task",
        "task-1",
        "task-auth-1",
        "2026-08-12T00:00:00Z",
    )
    .await
    .expect("store");
    let view = store.view();
    let call_id = agent_runtime::core::ids::ToolCallId::new("call-1");
    let history = vec![
        Message::user("inspect the task workspace"),
        Message::assistant(vec![agent_runtime::core::content::ContentPart::ToolCall(
            agent_runtime::core::content::ToolCall {
                id: call_id.clone(),
                name: "read_file".to_owned(),
                arguments: serde_json::json!({"path": "src/lib.rs"}),
            },
        )]),
        Message::tool_result(agent_runtime::core::content::ToolResultBlock {
            call_id,
            name: "read_file".to_owned(),
            content: vec![agent_runtime::core::content::ContentPart::text(
                "file contents",
            )],
            is_error: false,
        }),
        Message::assistant(vec![agent_runtime::core::content::ContentPart::text(
            "the task is ready for review",
        )]),
    ];
    let policy = TaskLcmProjectionPolicy::new("task-runtime-v1")
        .with_sensitivity(Sensitivity::Sensitive)
        .with_guard_revision(ContentGuardRevision::new("forge-guard-v1"))
        .with_transformation_revision(RegistryRevision::new("task-redaction-v1"));
    let result = store
        .project_task_history(&view, "task-projection-1", &history, &policy)
        .await
        .expect("projection");
    assert_eq!(result.entries, history.len());
    let replay = store
        .project_task_history(&view, "task-projection-1", &history, &policy)
        .await
        .expect("idempotent projection replay");
    assert!(replay.already_committed);
    let mut altered = history.clone();
    altered[0] = Message::user("different payload cannot reuse operation id");
    assert!(matches!(
        store
            .project_task_history(&view, "task-projection-1", &altered, &policy)
            .await,
        Err(agent_runtime::lcm::LcmError::IdempotencyConflict)
    ));
    let entries = store
        .load_range(
            &view,
            LcmRange::new(LcmSequence::new(0), LcmSequence::new(3)).unwrap(),
            8,
        )
        .await
        .expect("projected entries");
    assert_eq!(entries.len(), history.len());
    assert_eq!(
        entries[1].source.classification.trust,
        TrustClass::ExternalContent
    );
    assert_eq!(
        entries[2].source.classification.trust,
        TrustClass::ToolOutput
    );
    assert_eq!(
        entries[2].source.classification.guard_revision,
        Some(ContentGuardRevision::new("forge-guard-v1"))
    );
    assert_eq!(
        entries[2]
            .source
            .classification
            .transformation_revision
            .as_ref()
            .map(RegistryRevision::as_str),
        Some("task-redaction-v1")
    );

    let incomplete = vec![Message::assistant(vec![
        agent_runtime::core::content::ContentPart::ToolCall(
            agent_runtime::core::content::ToolCall {
                id: agent_runtime::core::ids::ToolCallId::new("call-incomplete"),
                name: "read_file".to_owned(),
                arguments: serde_json::json!({"path": "README.md"}),
            },
        ),
    ])];
    assert!(matches!(
        store
            .project_task_history(&view, "task-projection-incomplete", &incomplete, &policy)
            .await,
        Err(agent_runtime::lcm::LcmError::Invalid { .. })
    ));
    let secret_policy =
        TaskLcmProjectionPolicy::new("task-runtime-v1").with_sensitivity(Sensitivity::Secret);
    assert!(matches!(
        store
            .project_task_history(
                &view,
                "task-projection-secret",
                &[Message::user("secret")],
                &secret_policy,
            )
            .await,
        Err(agent_runtime::lcm::LcmError::SecretSource)
    ));
}

#[tokio::test]
async fn runtime_session_rotation_restart_preserves_scope_timeline_and_isolation() {
    let pool = db::create_sqlite_pool("sqlite::memory:")
        .await
        .expect("pool");
    db::run_migrations(&pool).await.expect("migrations");
    let db = Arc::new(SqliteDb::new(pool));
    let now = now_rfc3339();
    let identity_id = "continuity-identity".to_owned();
    let profile_id = "continuity-profile".to_owned();
    let project_id = "continuity-project".to_owned();
    let task_id = "continuity-task".to_owned();
    ProjectRepo::create(
        &*db,
        CreateProject {
            id: project_id.clone(),
            name: "Continuity project".to_owned(),
            settings: "{}".to_owned(),
            workflow_definition: "{}".to_owned(),
            primary_repo_id: None,
            owner_id: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("project");
    let chat_id = AgentChatRepo::get_project_chat(&*db, &project_id)
        .await
        .expect("Project Agent Chat reads")
        .expect("project creation provisions the singular Agent Chat")
        .id;
    TaskRepo::create(
        &*db,
        CreateTask {
            id: task_id.clone(),
            project_id: project_id.clone(),
            repo_id: None,
            parent_task_id: None,
            assignee_type: None,
            assignee_id: None,
            title: "Continuity task".to_owned(),
            description: Some("continuity fixture".to_owned()),
            task_type: "task".to_owned(),
            status: "todo".to_owned(),
            is_automation: false,
            priority: 0,
            subtask_order: None,
            task_state_config: None,
            merge_config: None,
            plan: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("task");
    AgentRepo::create_identity_with_profile(
        &*db,
        CreateAgentIdentity {
            id: identity_id.clone(),
            name: "Continuity identity".to_owned(),
            description: None,
            max_concurrent_tasks: 1,
            heartbeat_interval_seconds: 30,
            max_missed_heartbeats: 3,
            status: AgentStatus::Idle,
            last_heartbeat_at: None,
            is_default: false,
            paused: false,
            owner_id: None,
            visibility: "account".to_owned(),
            account_permission_ceiling: "{}".to_owned(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
        CreateAgentProfile {
            id: profile_id.clone(),
            identity_id: identity_id.clone(),
            backend_kind: "native".to_owned(),
            executor_type: "embedded".to_owned(),
            provider: Some("test".to_owned()),
            model: Some("test".to_owned()),
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
        },
    )
    .await
    .expect("identity/profile");
    AgentContextScopeRepo::create_context_scope(
        &*db,
        CreateAgentContextScope {
            id: "continuity-chat-scope".to_owned(),
            identity_id: identity_id.clone(),
            scope_type: "agent_chat".to_owned(),
            scope_id: chat_id.clone(),
            project_id: Some(project_id.clone()),
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
    .expect("Agent Chat scope");
    AgentContextScopeRepo::create_context_scope(
        &*db,
        CreateAgentContextScope {
            id: "continuity-task-scope".to_owned(),
            identity_id: identity_id.clone(),
            scope_type: "task".to_owned(),
            scope_id: task_id.clone(),
            project_id: Some(project_id.clone()),
            task_id: Some(task_id.clone()),
            task_role: Some("worker".to_owned()),
            workspace_access: "task_write".to_owned(),
            workspace_path: None,
            authority_json: "{}".to_owned(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("task scope");
    AgentSessionRepo::create_agent_session(
        &*db,
        CreateAgentSession {
            id: "continuity-chat-session-1".to_owned(),
            identity_id: identity_id.clone(),
            profile_id: profile_id.clone(),
            context_scope_id: "continuity-chat-scope".to_owned(),
            backend_kind: "native".to_owned(),
            runtime_session_id: Some("continuity-chat-runtime-1".to_owned()),
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
    .expect("Agent Chat session");
    AgentSessionRepo::create_agent_session(
        &*db,
        CreateAgentSession {
            id: "continuity-task-session-1".to_owned(),
            identity_id: identity_id.clone(),
            profile_id: profile_id.clone(),
            context_scope_id: "continuity-task-scope".to_owned(),
            backend_kind: "native".to_owned(),
            runtime_session_id: Some("continuity-task-runtime-1".to_owned()),
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
    .expect("task session");

    let protected = SqliteProtectedRuntimeStore::new(Arc::clone(&db), [7_u8; 32], 1);
    let chat_before = protected
        .lcm_store_for_runtime_session("continuity-chat-runtime-1", "agent_chat", &chat_id)
        .await
        .expect("Agent Chat runtime binding");
    let chat_view = chat_before.view();
    let entry = LcmEntry::new(
        LcmTimelineId::new(chat_before.timeline_id()),
        LcmEntryId::new("continuity-entry"),
        LcmSequence::new(0),
        Message::user("Agent Chat continuity record"),
        LcmSourceMetadata::new(LcmClassification::new(
            Sensitivity::Sensitive,
            TrustClass::UserContent,
        )),
    );
    chat_before
        .append(
            &chat_view,
            LcmAppendRequest::new(
                LcmOperationId::new("continuity-append"),
                vec![entry.clone()],
            ),
        )
        .await
        .expect("Agent Chat append");

    let replacement = AgentSessionRepo::rotate_agent_session(
        &*db,
        RotateAgentSession {
            previous_session_id: "continuity-chat-session-1".to_owned(),
            expected_version: 1,
            replacement: CreateAgentSession {
                id: "continuity-chat-session-2".to_owned(),
                identity_id: identity_id.clone(),
                profile_id: profile_id.clone(),
                context_scope_id: "continuity-chat-scope".to_owned(),
                backend_kind: "native".to_owned(),
                runtime_session_id: Some("continuity-chat-runtime-2".to_owned()),
                status: "ready".to_owned(),
                capabilities_json: "{}".to_owned(),
                connection_status: "healthy".to_owned(),
                predecessor_session_id: Some("continuity-chat-session-1".to_owned()),
                last_activity_at: None,
                created_at: now.clone(),
                updated_at: now.clone(),
            },
        },
    )
    .await
    .expect("rotate Agent Chat session");
    assert_eq!(
        replacement.predecessor_session_id.as_deref(),
        Some("continuity-chat-session-1")
    );

    let chat_after = protected
        .lcm_store_for_runtime_session("continuity-chat-runtime-2", "agent_chat", &chat_id)
        .await
        .expect("rotated Agent Chat runtime binding");
    assert_eq!(chat_before.timeline_id(), chat_after.timeline_id());
    assert_eq!(
        chat_after
            .current_revision(&chat_after.view())
            .await
            .unwrap()
            .get(),
        1
    );
    assert_eq!(
        chat_after
            .load_range(&chat_after.view(), LcmRange::single(LcmSequence::new(0)), 4)
            .await
            .unwrap(),
        vec![entry]
    );

    let restarted = SqliteProtectedRuntimeStore::new(Arc::clone(&db), [7_u8; 32], 1);
    let chat_after_restart = restarted
        .lcm_store_for_runtime_session("continuity-chat-runtime-2", "agent_chat", &chat_id)
        .await
        .expect("restart Agent Chat runtime binding");
    assert_eq!(chat_after.timeline_id(), chat_after_restart.timeline_id());

    let task_before = restarted
        .lcm_store_for_runtime_session("continuity-task-runtime-1", "task", &task_id)
        .await
        .expect("task runtime binding");
    let task_replacement = AgentSessionRepo::rotate_agent_session(
        &*db,
        RotateAgentSession {
            previous_session_id: "continuity-task-session-1".to_owned(),
            expected_version: 1,
            replacement: CreateAgentSession {
                id: "continuity-task-session-2".to_owned(),
                identity_id,
                profile_id,
                context_scope_id: "continuity-task-scope".to_owned(),
                backend_kind: "native".to_owned(),
                runtime_session_id: Some("continuity-task-runtime-2".to_owned()),
                status: "ready".to_owned(),
                capabilities_json: "{}".to_owned(),
                connection_status: "healthy".to_owned(),
                predecessor_session_id: Some("continuity-task-session-1".to_owned()),
                last_activity_at: None,
                created_at: now.clone(),
                updated_at: now,
            },
        },
    )
    .await
    .expect("rotate task session");
    assert_eq!(
        task_replacement.predecessor_session_id.as_deref(),
        Some("continuity-task-session-1")
    );
    let task_after = restarted
        .lcm_store_for_runtime_session("continuity-task-runtime-2", "task", &task_id)
        .await
        .expect("rotated task runtime binding");
    assert_eq!(task_before.timeline_id(), task_after.timeline_id());
    assert_eq!(
        task_after
            .current_revision(&task_after.view())
            .await
            .unwrap()
            .get(),
        0
    );
    assert_ne!(chat_after.timeline_id(), task_after.timeline_id());
    let restarted_again = SqliteProtectedRuntimeStore::new(Arc::clone(&db), [7_u8; 32], 1);
    let task_after_restart = restarted_again
        .lcm_store_for_runtime_session("continuity-task-runtime-2", "task", &task_id)
        .await
        .expect("restart task runtime binding");
    assert_eq!(task_after.timeline_id(), task_after_restart.timeline_id());
    assert!(matches!(
        task_after.current_revision(&chat_after.view()).await,
        Err(agent_runtime::lcm::LcmError::Unauthorized)
    ));
    let cross_scope_entry = LcmEntry::new(
        LcmTimelineId::new(chat_after.timeline_id()),
        LcmEntryId::new("continuity-cross-scope-entry"),
        LcmSequence::new(1),
        Message::user("must not enter the Task timeline"),
        LcmSourceMetadata::new(LcmClassification::new(
            Sensitivity::Sensitive,
            TrustClass::UserContent,
        )),
    );
    assert!(matches!(
        task_after
            .append(
                &task_after.view(),
                LcmAppendRequest::new(
                    LcmOperationId::new("continuity-cross-scope-append"),
                    vec![cross_scope_entry],
                ),
            )
            .await,
        Err(agent_runtime::lcm::LcmError::CrossTimeline)
    ));
    assert_eq!(
        task_after
            .current_revision(&task_after.view())
            .await
            .unwrap()
            .get(),
        0
    );
    assert!(
        restarted
            .lcm_store_for_runtime_session("continuity-chat-runtime-2", "task", &task_id)
            .await
            .is_err()
    );
    assert!(
        restarted
            .lcm_store_for_runtime_session(chat_after.timeline_id(), "agent_chat", &chat_id)
            .await
            .is_err()
    );
}

#[test]
fn runtime_manifest_link_is_reproducible_and_body_free() {
    let manifest = RunManifest::new(
        Fingerprint::of("registry"),
        Fingerprint::of("scope"),
        ModelResolution::new(
            "provider",
            ModelId::new("model"),
            Fingerprint::of("profile"),
            BTreeMap::new(),
        ),
        CapabilityResolution::new(RegistryRevision::new("capability-v1")),
        Fingerprint::of("context"),
        Fingerprint::of("cache"),
    )
    .with_segments(vec![ContextSegmentRecord::new(
        "task-segment",
        "task_description",
        SegmentSensitivity::Sensitive,
        Fingerprint::of("known-secret-body"),
        12,
    )])
    .with_summaries(vec![SummaryCoverage::new(
        SegmentId::new("summary-segment"),
        vec![SegmentId::new("task-segment")],
    )]);
    let turn = TurnManifest::new(TurnId::new("turn-1"), manifest.clone());
    let first = RuntimeContextManifestLink::from_turn_manifest(&turn).with_lcm_binding(
        "opaque-timeline",
        "authorization-v1",
        "forge-sqlite-lcm-1",
    );
    let second = RuntimeContextManifestLink::from_turn_manifest(&turn).with_lcm_binding(
        "opaque-timeline",
        "authorization-v1",
        "forge-sqlite-lcm-1",
    );
    assert_eq!(first, second);
    assert_eq!(
        first.runtime_manifest_fingerprint,
        manifest.fingerprint().as_str()
    );
    assert_eq!(first.summaries[0].covered, vec!["task-segment"]);
    let serialized = serde_json::to_string(&first).expect("manifest link serializes");
    assert!(!serialized.contains("known-secret-body"));
    assert!(!serialized.contains("provider-secret"));
    assert!(serialized.contains("content_hash"));
}

#[tokio::test]
async fn provisional_tail_truncation_removes_orphans_but_never_node_covered_entries() {
    let pool = db::create_sqlite_pool("sqlite::memory:")
        .await
        .expect("pool");
    db::run_migrations(&pool).await.expect("migrations");
    let db = Arc::new(SqliteDb::new(pool));
    AgentRepo::create(
        &*db,
        CreateAgent {
            id: "truncate-agent".to_owned(),
            name: "Truncate Agent".to_owned(),
            description: None,
            executor_type: "codex".to_owned(),
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
            owner_id: None,
            visibility: "account".to_owned(),
            created_at: "2026-08-12T00:00:00Z".to_owned(),
            updated_at: "2026-08-12T00:00:00Z".to_owned(),
        },
    )
    .await
    .expect("identity");

    let store = SqliteLcmStore::open_for_binding(
        Arc::clone(&db),
        "truncate-agent",
        "account",
        "account-1",
        "test-auth",
        "2026-08-12T00:00:00Z",
    )
    .await
    .expect("store");
    let view = store.view();

    let classification = || {
        LcmSourceMetadata::new(LcmClassification::new(
            agent_runtime::context::Sensitivity::Internal,
            TrustClass::UserContent,
        ))
    };
    let entries = (0..3)
        .map(|sequence| {
            LcmEntry::new(
                LcmTimelineId::new(store.timeline_id()),
                LcmEntryId::new(format!("entry-{sequence}")),
                LcmSequence::new(sequence),
                Message::user(format!("message {sequence}")),
                classification(),
            )
        })
        .collect::<Vec<_>>();
    store
        .append(
            &view,
            LcmAppendRequest::new(LcmOperationId::new("append-0"), entries.clone()),
        )
        .await
        .expect("append");

    // Orphan tail (entries 1..) is removable while no node covers it.
    let truncated = store
        .truncate_from(&view, LcmSequence::new(1))
        .await
        .expect("truncate provisional tail");
    assert_eq!(truncated.removed, 2);
    assert_eq!(truncated.revision.get(), 2);
    assert_eq!(
        store
            .load_range(
                &view,
                LcmRange::new(LcmSequence::new(0), LcmSequence::new(2)).unwrap(),
                8
            )
            .await
            .unwrap()
            .len(),
        1
    );

    // Truncating an already-empty tail is a no-op that keeps the revision.
    let noop = store
        .truncate_from(&view, LcmSequence::new(1))
        .await
        .expect("empty tail truncation");
    assert_eq!(noop.removed, 0);
    assert_eq!(noop.revision.get(), 2);

    // Once a node covers an entry, that entry can never be truncated.
    let node = LeafCommit {
        expected_revision: noop.revision,
        operation_id: LcmOperationId::new("leaf-0"),
        node_id: LcmNodeId::new("node-0"),
        range: LcmRange::single(LcmSequence::new(0)),
        entry_ids: vec![entries[0].id.clone()],
        source_fingerprint: source_fingerprint_entries(std::slice::from_ref(&entries[0])),
        summary: "covered".to_owned(),
        token_count: 1,
        source_token_count: 8,
        policy_revision: RegistryRevision::new("policy-1"),
        algorithm_revision: RegistryRevision::new("algorithm-1"),
        sizer_revision: RegistryRevision::new("sizer-1"),
        provenance: SummaryProvenance::Deterministic {
            revision: RegistryRevision::new("deterministic-1"),
        },
        classification: LcmClassification::new(
            agent_runtime::context::Sensitivity::Internal,
            TrustClass::UserContent,
        ),
        operation_fingerprint: None,
    };
    store.commit_leaf(&view, node).await.expect("leaf");
    assert!(matches!(
        store.truncate_from(&view, LcmSequence::new(0)).await,
        Err(agent_runtime::lcm::LcmError::RangeOverlap)
    ));
}
